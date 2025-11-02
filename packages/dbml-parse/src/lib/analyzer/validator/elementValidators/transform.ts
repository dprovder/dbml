/* eslint-disable class-methods-use-this */
import SymbolFactory from '../../symbol/factory';
import { CompileError, CompileErrorCode } from '../../../errors';
import {
  BlockExpressionNode, ElementDeclarationNode, FunctionApplicationNode, ProgramNode, SyntaxNode,
} from '../../../parser/nodes';
import { SyntaxToken } from '../../../lexer/tokens';
import { ElementValidator } from '../types';
import SymbolTable from '../../symbol/symbolTable';
import { destructureComplexVariable } from '../../utils';
import { createTableSymbolIndex } from '../../symbol/symbolIndex';

export default class TransformValidator implements ElementValidator {
  private declarationNode: ElementDeclarationNode & { type: SyntaxToken; };
  private publicSymbolTable: SymbolTable;
  private symbolFactory: SymbolFactory;

  constructor (declarationNode: ElementDeclarationNode & { type: SyntaxToken }, publicSymbolTable: SymbolTable, symbolFactory: SymbolFactory) {
    this.declarationNode = declarationNode;
    this.publicSymbolTable = publicSymbolTable;
    this.symbolFactory = symbolFactory;
  }

  validate (): CompileError[] {
    return [
      ...this.validateContext(),
      ...this.validateName(this.declarationNode.name),
      ...this.validateBody(this.declarationNode.body),
    ];
  }

  private validateContext (): CompileError[] {
    // Transforms can only appear at the top level (global scope)
    if (!(this.declarationNode.parent instanceof ProgramNode)) {
      return [new CompileError(
        CompileErrorCode.INVALID_TABLE_CONTEXT,
        'A Transform can only appear at the global scope',
        this.declarationNode,
      )];
    }

    return [];
  }

  private validateName (nameNode?: SyntaxNode): CompileError[] {
    // For MVP, we use minimal validation
    // The interpreter will handle detailed name validation
    if (!nameNode) {
      return [new CompileError(CompileErrorCode.INVALID_NAME, 'Transform must have a name', this.declarationNode)];
    }

    // Try to extract name, but don't fail if it's not a simple variable
    const nameFragments = destructureComplexVariable(nameNode);
    if (nameFragments.isOk()) {
      const names = nameFragments.unwrap();
      const trueName = names.join('.');

      // Register transform in symbol table (using same index structure as tables)
      const transformId = createTableSymbolIndex(trueName);

      if (this.publicSymbolTable.has(transformId)) {
        return [new CompileError(CompileErrorCode.DUPLICATE_NAME, `Transform "${trueName}" has already been defined`, nameNode)];
      }

      this.publicSymbolTable.set(transformId, this.declarationNode.symbol!);
    }

    // If we can't parse the name, that's okay for MVP - let interpreter handle it
    return [];
  }

  private validateBody (body?: FunctionApplicationNode | BlockExpressionNode): CompileError[] {
    // For MVP, we don't validate the body contents deeply
    // The interpreter will handle validation of transform-specific syntax
    if (!body) {
      return [new CompileError(CompileErrorCode.UNEXPECTED_TOKEN, 'Transform must have a body', this.declarationNode)];
    }

    if (!(body instanceof BlockExpressionNode)) {
      return [new CompileError(CompileErrorCode.UNEXPECTED_TOKEN, 'Transform body must be a block', this.declarationNode)];
    }

    // Transform bodies can contain:
    // - TransformColumnNode (column expressions with attributes)
    // - TransformStatementNode (join, where, group_by, etc.)
    // For now, we allow any content and let the interpreter validate

    return [];
  }
}
